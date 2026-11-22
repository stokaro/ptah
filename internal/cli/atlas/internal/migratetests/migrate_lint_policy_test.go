package migratetests_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/internal/exitcode"
)

// lintDroppedTable lints a migration that drops a table under policy, with
// comment on the line above the drop.
func lintDroppedTable(c *qt.C, policy atlascompatpolicy.Policy, comment string) (stdout string, code int) {
	c.Helper()
	dir := c.TB.TempDir()
	devDB := "sqlite://" + filepath.Join(c.TB.TempDir(), "dev.db")
	writeAtlasLintFile(c, dir, "1.sql", "CREATE TABLE users (id int);\n")
	writeAtlasLintFile(c, dir, "2.sql", comment+"DROP TABLE users;\n")

	stdout, _, err := runAtlasMigrateLintUnder(policy, "migrate", "lint", "--dir", "file://"+dir, "--dev-url", devDB, "--latest", "1")

	return redactAtlasLintDurations(stdout), exitcode.Code(err, 0)
}

// TestMigrateLint_DefaultPolicyPrintsTheAtlasCode holds the default policy to
// the community binary's line for a rule that reports an Atlas check.
func TestMigrateLint_DefaultPolicyPrintsTheAtlasCode(t *testing.T) {
	c := qt.New(t)

	stdout, code := lintDroppedTable(c, atlascompatpolicy.Full(), "")

	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Contains, `L1: Dropping table "users" https://atlasgo.io/lint/analyzers#DS102`)
	c.Assert(stdout, qt.Not(qt.Contains), "destructive/drop-table")
}

// TestMigrateLint_StrictPolicyPrintsTheAtlasCodeAndKeepsTheFinding is the
// control for the policy: strict mode names the finding by its Atlas code,
// which proves the finding is still reported and not dropped to look strict.
func TestMigrateLint_StrictPolicyPrintsTheAtlasCodeAndKeepsTheFinding(t *testing.T) {
	c := qt.New(t)

	stdout, code := lintDroppedTable(c, atlascompatpolicy.StrictCE(), "")

	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Contains, `L1: Dropping table "users" https://atlasgo.io/lint/analyzers#DS102`)
	c.Assert(stdout, qt.Not(qt.Contains), "destructive/drop-table")
}

func TestMigrateLint_AtlasCodeNoLintSuppressesUnderBothPolicies(t *testing.T) {
	for name, policy := range map[string]atlascompatpolicy.Policy{
		"default": atlascompatpolicy.Full(),
		"strict":  atlascompatpolicy.StrictCE(),
	} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)

			stdout, code := lintDroppedTable(c, policy, "-- atlas:nolint DS102\n")

			c.Assert(code, qt.Equals, 0)
			c.Assert(stdout, qt.Contains, "no diagnostics found")
		})
	}
}

func TestMigrateLint_RuleNameNoLintIsHonoredByDefaultOnly(t *testing.T) {
	c := qt.New(t)

	defaultOut, defaultCode := lintDroppedTable(c, atlascompatpolicy.Full(), "-- atlas:nolint destructive/drop-table\n")
	strictOut, strictCode := lintDroppedTable(c, atlascompatpolicy.StrictCE(), "-- atlas:nolint destructive/drop-table\n")

	c.Assert(defaultCode, qt.Equals, 0)
	c.Assert(defaultOut, qt.Contains, "no diagnostics found")
	// The community binary reads a rule name as a selector that matches
	// nothing, and so does strict mode.
	c.Assert(strictCode, qt.Equals, 1)
	c.Assert(strictOut, qt.Contains, "#DS102")
}
