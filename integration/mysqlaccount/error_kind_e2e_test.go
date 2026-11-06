//go:build integration

package mysqlaccount_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// missingPrivilege is one entry of an error's missing_privileges, read by its wire
// names.
type missingPrivilege struct {
	Database  string   `json:"database"`
	Objects   string   `json:"objects"`
	Names     []string `json:"names"`
	Privilege string   `json:"privilege"`
}

// failureDocument is what a caller parses from a failed run's standard output.
type failureDocument struct {
	Outcome string `json:"outcome"`
	Error   *struct {
		Kind              string             `json:"kind"`
		Message           string             `json:"message"`
		MissingPrivileges []missingPrivilege `json:"missing_privileges"`
	} `json:"error"`
}

// readDocument parses one run's document, which every verb prints on the failing
// path as well as the successful one.
func readDocument(c *qt.C, got clirun.Result) failureDocument {
	c.Helper()
	var document failureDocument
	c.Assert(json.Unmarshal([]byte(got.Stdout), &document), qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(document.Error, qt.IsNotNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	return document
}

// hiddenFromSelect is what an account holding SELECT on database lacks there, on
// each engine.
func hiddenFromSelect(database string) map[string][]missingPrivilege {
	onDatabase := "`" + database + "`.*"
	return map[string][]missingPrivilege{
		"MySQL": {
			{Database: database, Objects: "views", Names: []string{"v_child"}, Privilege: "SELECT and SHOW VIEW on " + onDatabase},
			{Database: database, Objects: "triggers", Privilege: "TRIGGER on " + onDatabase},
			{Database: database, Objects: "routines", Privilege: "SHOW_ROUTINE on *.*"},
		},
		"MariaDB": {
			{Database: database, Objects: "views", Names: []string{"v_child"}, Privilege: "SELECT and SHOW VIEW on " + onDatabase},
			{Database: database, Objects: "constraints", Privilege: "a privilege beyond SELECT, such as SHOW VIEW, on " + onDatabase},
			{Database: database, Objects: "triggers", Privilege: "TRIGGER on " + onDatabase},
			{Database: database, Objects: "routines", Privilege: "SHOW CREATE ROUTINE on " + onDatabase},
		},
	}
}

// TestARefusedReadOfTheTargetNamesItsKindE2E is what a caller reads about an account
// that cannot see what the command depends on, from each document that can report
// it: the kind and the privileges, with the exit code and the sentence the refusal
// has without them. The caller it is for reads the kind and never the message.
func TestARefusedReadOfTheTargetNamesItsKindE2E(t *testing.T) {
	verbs := []struct {
		name        string
		args        []string
		wantOutcome string
	}{
		{name: "schema plan", args: []string{"schema", "plan", "--dry-run", "--json"}, wantOutcome: "failed"},
		{name: "schema apply", args: []string{"schema", "apply", "--auto-approve", "--json"}, wantOutcome: "failed"},
		{name: "schema drift", args: []string{"schema", "drift", "--format", "json"}},
	}
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s)
			file := filepath.Join(c.TempDir(), "desired.sql")
			c.Assert(os.WriteFile(file, []byte(strings.Join(sharedObjects, ";\n")+";\n"), 0o600), qt.IsNil)
			account := f.newAccount(c, "kind", "GRANT SELECT ON {db}.* TO {account}")
			for _, verb := range verbs {
				t.Run(verb.name, func(t *testing.T) {
					c := qt.New(t)
					args := slices.Concat(verb.args, []string{"--db-url", f.url(account, f.left), "--schema-file", file})

					got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)

					c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
					document := readDocument(c, got)
					c.Assert(document.Outcome, qt.Equals, verb.wantOutcome)
					c.Assert(document.Error.Kind, qt.Equals, "permission_denied")
					c.Assert(document.Error.MissingPrivileges, qt.DeepEquals, hiddenFromSelect(f.left)[s.name])
					c.Assert(document.Error.Message, qt.Contains, "cannot see everything")
					c.Assert(got.Stderr, qt.Contains, "cannot see everything")
				})
			}
		})
	}
}

// TestARefusedReadBeforeASavedPlanNamesItsKindE2E takes the other read of the
// target: an administrator saved the plan, and the account that applies it cannot
// see what the plan's fingerprint is checked against.
func TestARefusedReadBeforeASavedPlanNamesItsKindE2E(t *testing.T) {
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s)
			dir := c.TempDir()
			file := filepath.Join(dir, "desired.sql")
			c.Assert(os.WriteFile(file, []byte(strings.Join(sharedObjects, ";\n")+";\n"), 0o600), qt.IsNil)
			saved := filepath.Join(dir, "saved.plan.json")
			planned := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
				"schema", "plan", "--db-url", f.url("", f.left), "--schema-file", file, "--output", saved)
			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			account := f.newAccount(c, "saved", "GRANT SELECT ON {db}.* TO {account}")

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
				"schema", "apply", "--db-url", f.url(account, f.left), "--plan", saved, "--auto-approve", "--json")

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			document := readDocument(c, got)
			c.Assert(document.Outcome, qt.Equals, "failed")
			c.Assert(document.Error.Kind, qt.Equals, "permission_denied")
			c.Assert(document.Error.MissingPrivileges, qt.DeepEquals, hiddenFromSelect(f.left)[s.name])
		})
	}
}

// TestARefusedReadOfAnotherDatabaseHasNoKindE2E is the control. The target opens and
// reads, and the same refusal comes from the database --to names, so it says nothing
// about the target a caller holds the kind against.
func TestARefusedReadOfAnotherDatabaseHasNoKindE2E(t *testing.T) {
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s)
			account := f.newAccount(c, "source", "GRANT SELECT ON {db}.* TO {account}")

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
				"schema", "apply", "--db-url", f.url("", f.left), "--to", f.url(account, f.right), "--dry-run", "--json")

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			document := readDocument(c, got)
			c.Assert(document.Outcome, qt.Equals, "failed")
			c.Assert(document.Error.Kind, qt.Equals, "")
			c.Assert(document.Error.MissingPrivileges, qt.IsNil)
			c.Assert(document.Error.Message, qt.Contains, "read --to database schema")
			c.Assert(document.Error.Message, qt.Contains, "cannot see everything")
		})
	}
}
