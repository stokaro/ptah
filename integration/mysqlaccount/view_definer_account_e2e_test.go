//go:build integration

package mysqlaccount_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestAPlanningVerbRefusesAViewTheAdministratorDefinedE2E covers a limited
// account meeting a SQL SECURITY DEFINER view another account defined. The
// administrator defines v_owned, the desired schema changes its body, and the
// account plans with every trigger and event left out, so the read itself is
// one the account may make. The replacement would make the account the view's
// definer, so both planning verbs refuse it and name the view, its definer and
// the account; the administrator, the view's definer, plans it.
func TestAPlanningVerbRefusesAViewTheAdministratorDefinedE2E(t *testing.T) {
	const everyTrigger = "*[type=trigger|event]"
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s, "CREATE SQL SECURITY DEFINER VIEW v_owned AS SELECT id FROM child")
			file := filepath.Join(c.TempDir(), "desired.sql")
			desired := slices.Concat(sharedObjects,
				[]string{"CREATE SQL SECURITY DEFINER VIEW v_owned AS SELECT id, amount FROM child"})
			c.Assert(os.WriteFile(file, []byte(strings.Join(desired, ";\n")+";\n"), 0o600), qt.IsNil)
			account := f.newAccount(c, "vdef", "GRANT SELECT, SHOW VIEW ON {db}.* TO {account}", s.routineGrant)

			for _, verb := range planningVerbs() {
				t.Run(verb.name, func(t *testing.T) {
					c := qt.New(t)

					refused, refusedSQL := verb.run(c, f.url(account, f.left), file, "--exclude", everyTrigger)
					planned, plannedSQL := verb.run(c, f.url("", f.left), file, "--exclude", everyTrigger)

					c.Assert(refused.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
					c.Assert(refused.Stderr, qt.Matches, `(?s).*view v_owned runs with the privileges of its definer \S+ `+
						`under SQL SECURITY DEFINER, and replacing it as `+account+`@% would make `+account+`@% its definer.*`)
					c.Assert(refusedSQL, qt.Equals, "")
					c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
					c.Assert(plannedSQL, qt.Contains, "VIEW `v_owned`")
				})
			}
		})
	}
}
