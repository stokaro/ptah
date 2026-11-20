package migrationlintreport_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/migrationlintreport"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// truncateAfterDrop holds one rule the Atlas table maps, destructive/drop-table,
// and one it does not, destructive/truncate-table.
var truncateAfterDrop = map[string]string{
	"1_init.sql":   "CREATE TABLE users (id int);\n",
	"2_hazard.sql": "TRUNCATE TABLE users;\nDROP TABLE users;\n",
}

func compatRulesOf(c *qt.C, vocabulary *migrationlintreport.AtlasVocabulary) []string {
	c.Helper()
	report, err := migrationlintreport.Build(c.Context(), migrationlintreport.Options{
		Runtime:       selectedRuntime(c),
		Dir:           writeMigrations(c, truncateAfterDrop),
		DirFormat:     string(migrationfile.DirFormatAtlas),
		Dialect:       "postgres",
		FailOn:        migrationlintreport.FailOnNone,
		Compatibility: lint.CompatibilityProfileAtlas,
		Atlas:         vocabulary,
		Changed:       migrationlintreport.ChangedOptions{Dialect: true},
	}, projectconfig.Config{})
	c.Assert(err, qt.IsNil)
	var rules []string
	for _, finding := range report.Findings {
		rules = append(rules, finding.Rule)
	}
	return rules
}

// TestBuild_DefaultCompatibilityRunsEveryRule is the control for the strict
// run below: the rule the table does not map is reported when nothing
// restricts the run, so its absence under the restriction is the restriction's
// doing and not a fixture that never produced it.
func TestBuild_DefaultCompatibilityRunsEveryRule(t *testing.T) {
	c := qt.New(t)

	rules := compatRulesOf(c, &migrationlintreport.AtlasVocabulary{})

	c.Assert(rules, qt.Contains, "destructive/truncate-table")
	c.Assert(rules, qt.Contains, "destructive/drop-table")
}

func TestBuild_StrictCompatibilityRunsOnlyTheMappedRules(t *testing.T) {
	c := qt.New(t)

	rules := compatRulesOf(c, &migrationlintreport.AtlasVocabulary{
		Only: func(name string) bool { return name == "destructive/drop-table" },
	})

	c.Assert(slices.Contains(rules, "destructive/truncate-table"), qt.IsFalse)
	c.Assert(rules, qt.Contains, "destructive/drop-table")
}
