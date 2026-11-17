//go:build integration

package migrateup_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testutils"
)

// These tests drive the shipped binary against PostgreSQL through a run whose
// only work is a changed repeatable migration. Such a run passes what every run
// checks before it changes anything: the destructive-change gate and the
// pre-up hook. Without that, a changed body that drops a table runs with no
// --allow-destructive, and a failing --pre-up-hook does not stop it. The
// tables and the record are read back from the server.

func TestRepeatableMigrationsGateE2E_HappyPath(t *testing.T) {
	t.Run("--allow-destructive runs a changed repeatable migration that drops a table", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)
		result, _ := r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		dropping := "DROP TABLE repeatable_audit;\n"
		r.write(c, "R__account_names.sql", dropping)

		result, document := r.up(c, "--allow-destructive")
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(document.AppliedRepeatables, qt.DeepEquals, []string{"account_names"})
		c.Assert(userTables(c, r.conn), qt.Not(qt.Contains), "repeatable_audit")
		c.Assert(r.status(c).Repeatables[0].AppliedChecksum, qt.Equals, e2eH1(dropping))
	})
}

func TestRepeatableMigrationsGateE2E_FailurePath(t *testing.T) {
	t.Run("a changed repeatable migration that drops a table is refused without --allow-destructive", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)
		result, _ := r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		r.write(c, "R__account_names.sql", "DROP TABLE repeatable_audit;\n")

		result = r.ptah(c, "migrations", "up", "--db-url", r.url, "--migrations-dir", r.dir)
		c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
		c.Assert(result.Stderr, qt.Matches,
			`(?s).*pending migrations contain destructive statements.*\n- .*R__account_names\.sql:1 DS101 error: .*`)
		c.Assert(userTables(c, r.conn), qt.Contains, "repeatable_audit")
		c.Assert(r.audit(c), qt.DeepEquals, []string{"v1"})
		c.Assert(r.status(c).Repeatables[0].AppliedChecksum, qt.Equals, e2eH1(repeatableE2EView("v1", "name")))
	})

	t.Run("a failing pre-up hook stops a run whose only work is a repeatable migration", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)
		result, _ := r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		r.write(c, "R__account_names.sql", repeatableE2EView("v2", "name, id"))

		result = r.ptah(c, "migrations", "up", "--db-url", r.url, "--migrations-dir", r.dir,
			"--pre-up-hook", testutils.FailingHookCommand("blocked", 7))
		c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
		c.Assert(result.Stderr, qt.Contains, "up pre-flight custom command hook failed")
		c.Assert(r.audit(c), qt.DeepEquals, []string{"v1"})
		c.Assert(r.viewColumns(c), qt.DeepEquals, []string{"name"})
	})
}
