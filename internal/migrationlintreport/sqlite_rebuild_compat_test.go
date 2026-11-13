package migrationlintreport_test

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/migrationlintreport"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// The first migration supplies the old column list through replay. The last
// migration keeps every row but omits name from the replacement table.
func TestBuild_AtlasSQLiteRebuildReportsDroppedColumn(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	files := map[string]string{
		"1_initial.sql": "CREATE TABLE users (id integer PRIMARY KEY, name text);\n",
		"2_rebuild.sql": "CREATE TABLE new_users (id integer PRIMARY KEY);\n" +
			"INSERT INTO new_users (id) SELECT id FROM users;\n" +
			"DROP TABLE users;\nALTER TABLE new_users RENAME TO users;\n",
	}
	for name, sql := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil)
	}
	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Runtime: selectedRuntime(c), Dir: dir,
		DirFormat: string(migrationfile.DirFormatAtlas), Dialect: "sqlite",
		DevURL: "sqlite://" + filepath.Join(t.TempDir(), "dev.db"),
		Latest: 1, FailOn: migrationlintreport.FailOnError,
		Compatibility: lint.CompatibilityProfileAtlas,
		Changed:       migrationlintreport.ChangedOptions{Dialect: true, DevURL: true, Latest: true},
	}, projectconfig.Config{})
	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed, qt.IsTrue)
	destructive := slices.DeleteFunc(slices.Clone(report.Findings), func(finding lint.Finding) bool {
		return finding.Rule != "DS101" && finding.Rule != "DS102" && finding.Rule != "BC101"
	})
	c.Assert(destructive, qt.HasLen, 1)
	c.Assert(destructive[0].Rule, qt.Equals, "DS102")
	c.Assert(destructive[0].Context.Subjects[0].Name, qt.Equals, "name")
}
