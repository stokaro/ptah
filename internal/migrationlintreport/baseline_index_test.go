package migrationlintreport_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/migrationlintreport"
	"ptah.run/migration/migrationfile"
)

// writeIndexMigrations writes the directory every case below lints: version
// 1 creates a table with a plain index, version 2 drops that index and
// builds a unique one under another name. Nothing in version 2 says which
// columns the dropped index covered; the dev database, replayed to the state
// version 2 starts from, does.
func writeIndexMigrations(c *qt.C) string {
	c.Helper()
	dir := c.TB.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"),
		[]byte("CREATE TABLE orders (id integer, email text);\nCREATE INDEX orders_email_idx ON orders (email);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "2_unique.sql"),
		[]byte("DROP INDEX orders_email_idx;\nCREATE UNIQUE INDEX orders_email_uq ON orders (email);\n"), 0o600), qt.IsNil)
	return dir
}

func lintRules(report migrationlintreport.Report) []string {
	rules := make([]string, 0, len(report.Findings))
	for _, finding := range report.Findings {
		rules = append(rules, finding.Rule)
	}
	return rules
}

// TestBuild_DevURLSuppliesTheIndexesTheUniqueRulesRead drives the whole
// path a run takes: the replay on the dev database, the catalog read that
// carries the indexes beside the columns, and the rule that reads them.
func TestBuild_DevURLSuppliesTheIndexesTheUniqueRulesRead(t *testing.T) {
	c := qt.New(t)
	dir := writeIndexMigrations(c)

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:       dir,
		DirFormat: string(migrationfile.DirFormatAtlas),
		Dialect:   "sqlite",
		DevURL:    "sqlite://" + filepath.Join(t.TempDir(), "dev.db"),
		FailOn:    migrationlintreport.FailOnNone,
		Changed:   migrationlintreport.ChangedOptions{Dialect: true, DevURL: true},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(lintRules(report), qt.DeepEquals, []string{"MF102"})
	c.Assert(report.Findings[0].Message, qt.Contains,
		"replaces the index orders_email_idx dropped earlier, which covered the same columns, with a unique one under a new name")
	c.Assert(report.Analysis.UnmetInputs(), qt.HasLen, 0)
}

// TestBuild_WithoutADevURLTheUniqueRulesReportFromTheTextAndSaySo is the
// control: the same directory with no dev database reports the build as
// MF101 and names the refinement it went without.
func TestBuild_WithoutADevURLTheUniqueRulesReportFromTheTextAndSaySo(t *testing.T) {
	c := qt.New(t)
	dir := writeIndexMigrations(c)

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:       dir,
		DirFormat: string(migrationfile.DirFormatAtlas),
		Dialect:   "sqlite",
		FailOn:    migrationlintreport.FailOnNone,
		Changed:   migrationlintreport.ChangedOptions{Dialect: true},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(lintRules(report), qt.DeepEquals, []string{"MF101"})
	var unmet []string
	for _, entry := range report.Analysis.UnmetInputs() {
		unmet = append(unmet, entry.Rule)
	}
	c.Assert(unmet, qt.DeepEquals, []string{"MF101", "MF102"})
}
