//go:build integration

package mysqlaccount_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// An account that can read a procedure's body cannot necessarily read its
// privileges. Generating a removal must refuse before writing either direction
// when its rollback cannot capture those privileges. Excluding routines lets
// the same account generate the table changes without a routine removal.
func TestMigrationGenerationRefusesUnreadRoutinePrivilegesE2E(t *testing.T) {
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s, leftProcedure...)
			file := sharedObjectsFile(c)
			account := f.newAccount(c, "rollback", "GRANT SELECT, SHOW VIEW ON {db}.* TO {account}", s.routineGrant)
			dir := c.TempDir()
			args := []string{
				"migrations", "generate", "--db-url", f.url(account, f.left), "--schema-file", file,
				"--migrations-dir", dir, "--name", "second",
			}

			refused := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
				append(args, "--exclude", "*[type=trigger|event]")...)

			c.Assert(refused.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
			c.Assert(refused.Stderr, qt.Contains, "cannot plan the rollback of removing p_noop(): the database read could not read the routine privileges")
			c.Assert(refused.Stderr, qt.Contains, "SELECT on mysql.procs_priv")
			written, err := filepath.Glob(filepath.Join(dir, "*.sql"))
			c.Assert(err, qt.IsNil)
			c.Assert(written, qt.HasLen, 0)

			excluded := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
				append(args, "--exclude", "*[type=function|trigger|event]")...)

			c.Assert(excluded.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", excluded.Stdout, excluded.Stderr))
			ups, err := filepath.Glob(filepath.Join(dir, "*_second.up.sql"))
			c.Assert(err, qt.IsNil)
			c.Assert(ups, qt.HasLen, 1)
			up, err := os.ReadFile(ups[0])
			c.Assert(err, qt.IsNil)
			c.Assert(string(up), qt.Contains, "DROP TABLE `only_left`")
			c.Assert(string(up), qt.Not(qt.Contains), "p_noop")
			downs, err := filepath.Glob(filepath.Join(dir, "*_second.down.sql"))
			c.Assert(err, qt.IsNil)
			c.Assert(downs, qt.HasLen, 1)
			down, err := os.ReadFile(downs[0])
			c.Assert(err, qt.IsNil)
			c.Assert(string(down), qt.Contains, "CREATE TABLE `only_left`")
			c.Assert(string(down), qt.Not(qt.Contains), "p_noop")
		})
	}
}
