package migrationlintreport_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/migrationlintreport"
	"ptah.run/migration/migrationfile"
)

// These tests read which files the selectors give the analysis in a Ptah
// directory that holds repeatable migrations. A repeatable migration has no
// version, so --latest never selects one, and --git-base selects one by its
// path when its file changed.

func reportSelectedNames(report migrationlintreport.Report) []string {
	names := make([]string, 0)
	for _, file := range report.Analysis.SelectedFiles() {
		names = append(names, file.Name)
	}
	return names
}

func TestBuild_RepeatableSelection_HappyPath(t *testing.T) {
	t.Run("--latest selects versions and does not refuse a repeatable migration", func(t *testing.T) {
		c := qt.New(t)
		report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
			Runtime: selectedRuntime(c),
			Dir:     t.TempDir(),
			FS: fstest.MapFS{
				"0000000001_users.up.sql":    {Data: []byte("CREATE TABLE users (id int);\n")},
				"0000000001_users.down.sql":  {Data: []byte("SELECT 1;\n")},
				"0000000002_orders.up.sql":   {Data: []byte("CREATE TABLE orders (id int);\n")},
				"0000000002_orders.down.sql": {Data: []byte("SELECT 1;\n")},
				"R__drop_users.sql":          {Data: []byte("DROP TABLE users;\n")},
			},
			DirFormat: string(migrationfile.DirFormatPtah),
			Dialect:   "sqlite",
			FailOn:    migrationlintreport.FailOnNone,
			Latest:    1,
			Changed:   migrationlintreport.ChangedOptions{Latest: true},
		}, projectconfig.Config{})

		c.Assert(err, qt.IsNil)
		c.Assert(reportSelectedNames(report), qt.DeepEquals, []string{"0000000002_orders.down.sql", "0000000002_orders.up.sql"})
		c.Assert(report.Findings, qt.HasLen, 0)
	})

	t.Run("--git-base selects a changed repeatable migration by its path", func(t *testing.T) {
		c := qt.New(t)
		repo, err := filepath.EvalSymlinks(t.TempDir())
		c.Assert(err, qt.IsNil)
		migrationsDir := filepath.Join(repo, "migrations")
		c.Assert(os.MkdirAll(filepath.Join(migrationsDir, "views"), 0o700), qt.IsNil)
		runGit(c, repo, "init")
		runGit(c, repo, "config", "commit.gpgsign", "false")
		runGit(c, repo, "config", "user.email", "ptah@example.com")
		runGit(c, repo, "config", "user.name", "Ptah Test")
		writeLintTestFile(c, migrationsDir, "0000000001_users.up.sql", "CREATE TABLE users (id int);\n")
		writeLintTestFile(c, migrationsDir, "0000000001_users.down.sql", "DROP TABLE users;\n")
		writeLintTestFile(c, migrationsDir, "R__unchanged.sql", "DROP TABLE orders;\n")
		writeLintTestFile(c, migrationsDir, "views/R__names.sql", "SELECT 1;\n")
		runGit(c, repo, "add", "migrations")
		runGit(c, repo, "commit", "-m", "baseline")
		writeLintTestFile(c, migrationsDir, "views/R__names.sql", "DROP TABLE users;\n")
		runGit(c, repo, "commit", "-am", "repeatable")

		report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
			Runtime:   selectedRuntime(c),
			Dir:       migrationsDir,
			DirFormat: string(migrationfile.DirFormatPtah),
			Dialect:   "sqlite",
			GitBase:   "HEAD~1",
			GitDir:    repo,
			FailOn:    migrationlintreport.FailOnNone,
			Changed:   migrationlintreport.ChangedOptions{GitBase: true, GitDir: true},
		}, projectconfig.Config{})

		c.Assert(err, qt.IsNil)
		c.Assert(reportSelectedNames(report), qt.DeepEquals, []string{"views/R__names.sql"})
		c.Assert(findingRules(report.Findings), qt.DeepEquals, []string{"destructive/drop-table", "rollout/drop-table"})
		c.Assert(report.Findings[0].File, qt.Contains, "views/R__names.sql")
	})
}
