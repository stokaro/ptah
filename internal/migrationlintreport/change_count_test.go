package migrationlintreport_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/migrationlintreport"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// writeMigrations writes one Atlas-format directory holding files, by name.
func writeMigrations(c *qt.C, files map[string]string) string {
	c.Helper()
	dir := c.TB.TempDir()
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

// freshDevURL is a SQLite dev database nothing has written yet.
func freshDevURL(c *qt.C) string {
	c.Helper()
	return "sqlite://" + filepath.Join(c.TB.TempDir(), "dev.db")
}

// buildCount lints dir under count, against the dev database devURL names or
// none when it is empty, and returns the report.
func buildCount(c *qt.C, dir string, count lint.ChangeCount, devURL string) migrationlintreport.Report {
	c.Helper()
	report, err := migrationlintreport.Build(c.Context(), migrationlintreport.Options{
		Runtime:     selectedRuntime(c),
		Dir:         dir,
		DirFormat:   string(migrationfile.DirFormatAtlas),
		Dialect:     "sqlite",
		DevURL:      devURL,
		FailOn:      migrationlintreport.FailOnNone,
		ChangeCount: count,
		Changed:     migrationlintreport.ChangedOptions{Dialect: true, DevURL: devURL != ""},
	}, projectconfig.Config{})
	c.Assert(err, qt.IsNil)
	return report
}

// changesIn returns how many schema changes the named file of the report
// counts.
func changesIn(c *qt.C, report migrationlintreport.Report, name string) int {
	c.Helper()
	for _, file := range report.Analysis.Files() {
		if file.Name == name {
			return len(file.Changes)
		}
	}
	c.Fatalf("file %q not in the report", name)
	return 0
}

// firstFileDrop is a first version whose IF EXISTS drop finds nothing on an
// empty dev database.
var firstFileDrop = map[string]string{"1_init.sql": "DROP TABLE IF EXISTS missing;\nCREATE TABLE users (id int);\n"}

// The dev database is read for the first version too, where it holds nothing:
// the drop drops nothing, so it is neither counted nor reported, and no rule is
// named as missing its input. The drop rules ask for that state under every
// count, the native one included.
func TestBuild_DevURLReadsTheEmptyStateOfTheFirstVersion(t *testing.T) {
	counts := map[string]lint.ChangeCount{
		"statement count": lint.ChangeCountStatements,
		"Atlas count":     lint.ChangeCountAtlas,
		"community count": lint.ChangeCountAtlasCommunity,
	}
	for name, count := range counts {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			report := buildCount(c, writeMigrations(c, firstFileDrop), count, freshDevURL(c))
			c.Assert(changesIn(c, report, "1_init.sql"), qt.Equals, 1)
			c.Assert(lintRules(report), qt.HasLen, 0)
			c.Assert(report.Analysis.UnmetInputs(), qt.HasLen, 0)
		})
	}
}

// The control: without a dev database the drop is counted and reported.
func TestBuild_WithoutADevURLTheFirstVersionDropIsReported(t *testing.T) {
	c := qt.New(t)
	report := buildCount(c, writeMigrations(c, firstFileDrop), lint.ChangeCountAtlas, "")
	c.Assert(changesIn(c, report, "1_init.sql"), qt.Equals, 2)
	c.Assert(lintRules(report), qt.DeepEquals, []string{"BC103", "DS101"})
}

// The Atlas counts read the state of a version whose CREATE TABLE IF NOT
// EXISTS only that state decides, though no rule asks for it. The statement
// count reads nothing for it and counts the statement.
func TestBuild_ChangeCountReadsTheStateItDecidesWith(t *testing.T) {
	files := map[string]string{
		"1_users.sql": "CREATE TABLE users (id int);\n",
		"2_head.sql":  "CREATE TABLE IF NOT EXISTS users (id int);\n",
	}
	tests := []struct {
		name  string
		count lint.ChangeCount
		want  int
	}{
		{name: "Atlas count", count: lint.ChangeCountAtlas, want: 0},
		{name: "community count", count: lint.ChangeCountAtlasCommunity, want: 0},
		{name: "statement count", count: lint.ChangeCountStatements, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			report := buildCount(c, writeMigrations(c, files), test.count, freshDevURL(c))
			c.Assert(changesIn(c, report, "2_head.sql"), qt.Equals, test.want)
		})
	}
}
