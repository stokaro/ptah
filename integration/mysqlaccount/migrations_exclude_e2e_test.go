//go:build integration

package mysqlaccount_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// planningVerb is one verb that plans a migration from a database read, with the
// statements it planned apart from the comments that name the address it read.
type planningVerb struct {
	name string
	run  func(c *qt.C, address, file string, flags ...string) (clirun.Result, string)
}

// planningVerbs are `migrations plan` and `migrations generate`, which read the
// database through the same check `schema diff` does and take its --exclude.
func planningVerbs() []planningVerb {
	return []planningVerb{
		{
			name: "migrations plan",
			run: func(c *qt.C, address, file string, flags ...string) (clirun.Result, string) {
				c.Helper()
				got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
					append([]string{"migrations", "plan", "--db-url", address, "--schema-file", file}, flags...)...)
				_, planned, _ := strings.Cut(got.Stdout, "=== MIGRATION SQL ===\n")
				return got, withoutComments(planned)
			},
		},
		{
			name: "migrations generate",
			run: func(c *qt.C, address, file string, flags ...string) (clirun.Result, string) {
				c.Helper()
				dir := c.TempDir()
				got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, append([]string{
					"migrations", "generate", "--db-url", address, "--schema-file", file,
					"--migrations-dir", dir, "--name", "second",
				}, flags...)...)
				ups, err := filepath.Glob(filepath.Join(dir, "*_second.up.sql"))
				c.Assert(err, qt.IsNil)
				var planned strings.Builder
				for _, up := range ups {
					contents, err := os.ReadFile(up)
					c.Assert(err, qt.IsNil)
					planned.Write(contents)
				}
				return got, withoutComments(planned.String())
			},
		},
	}
}

// withoutComments drops the comment lines of planned SQL, which carry the
// address a run read and the time it wrote the file.
func withoutComments(planned string) string {
	var kept []string
	for line := range strings.Lines(planned) {
		if !strings.HasPrefix(line, "--") {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "")
}

// TestAPlanningVerbThatLeavesAKindOutDoesNotNeedItsPrivilegeE2E covers the read
// `migrations plan` and `migrations generate` make of the target. An --exclude
// that leaves every trigger out reaches the check that refuses a read of what an
// account cannot see, so an account holding no TRIGGER plans what the
// administrator's connection plans with the same selector. Without the selector
// the same account is refused, which shows the relief comes from the selector.
// Events are left out too, because a report names them for an account holding
// no EVENT and the administrator holds it.
func TestAPlanningVerbThatLeavesAKindOutDoesNotNeedItsPrivilegeE2E(t *testing.T) {
	const everyTrigger = "*[type=trigger|event]"
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s, leftProcedure...)
			file := sharedObjectsFile(c)
			account := f.newAccount(c, "plan", "GRANT SELECT, SHOW VIEW ON {db}.* TO {account}", s.routineGrant)
			address := f.url(account, f.left)
			administrator := f.url("", f.left)

			for _, verb := range planningVerbs() {
				t.Run(verb.name, func(t *testing.T) {
					c := qt.New(t)

					got, gotSQL := verb.run(c, address, file, "--exclude", everyTrigger)
					want, wantSQL := verb.run(c, administrator, file, "--exclude", everyTrigger)

					c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
					c.Assert(want.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", want.Stdout, want.Stderr))
					c.Assert(wantSQL, qt.Contains, "DROP TABLE `only_left`")
					c.Assert(gotSQL, qt.Equals, wantSQL)

					refused, refusedSQL := verb.run(c, address, file)
					c.Assert(refused.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
					c.Assert(refused.Stderr, qt.Contains, "cannot see everything")
					c.Assert(refusedSQL, qt.Equals, "")
				})
			}
		})
	}
}
