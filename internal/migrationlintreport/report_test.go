package migrationlintreport_test

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/internal/migrationlintreport"
	"github.com/stokaro/ptah/internal/migrationsnapshot"
	migrationlint "github.com/stokaro/ptah/migration/lint"
	"github.com/stokaro/ptah/migration/migrator"
)

func writeLintTestFile(c *qt.C, dir, name, content string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
}

func writeWarningMigration(c *qt.C, dir string) {
	c.Helper()
	writeLintTestFile(c, dir, "0000000001_index.up.sql", "CREATE INDEX idx_users_id ON users (id);\n")
	writeLintTestFile(c, dir, "0000000001_index.down.sql", "DROP INDEX idx_users_id;\n")
}

func TestBuild_UsesProjectConfigWithoutCobra(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	latest := 1
	writeLintTestFile(c, dir, "0000000001_old.up.sql", "DROP TABLE old_data;\n")
	writeLintTestFile(c, dir, "0000000001_old.down.sql", "CREATE TABLE old_data (id INT);\n")
	writeLintTestFile(c, dir, "0000000002_new.up.sql", "ALTER TABLE users DROP COLUMN legacy;\n")
	writeLintTestFile(c, dir, "0000000002_new.down.sql", "ALTER TABLE users ADD COLUMN legacy TEXT;\n")

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:       dir,
		DirFormat: string(migrator.MigrationDirFormatPtah),
		FailOn:    migrationlintreport.FailOnNone,
	}, projectconfig.Config{
		Lint: projectconfig.LintConfig{
			Dialect:       "postgres",
			DisabledRules: []string{"MF"},
			Latest:        &latest,
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed, qt.IsFalse)
	c.Assert(report.Dialect, qt.Equals, "postgres")
	c.Assert(report.DisabledRules, qt.DeepEquals, []string{"MF"})
	c.Assert(report.Versions, qt.DeepEquals, []int64{2})
	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].Rule, qt.Equals, "DS102")
	c.Assert(report.Findings[0].File, qt.Contains, "0000000002_new.up.sql")
}

func TestBuild_ExplicitEmptyProjectDirReachesValidation(t *testing.T) {
	c := qt.New(t)
	cfg, err := projectconfig.ParsePtah([]byte(`
migration:
  dir: ""
`), "ptah.yaml", "")
	c.Assert(err, qt.IsNil)

	_, err = migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:       t.TempDir(),
		DirFormat: string(migrator.MigrationDirFormatAtlas),
		Dialect:   "sqlite",
		FailOn:    migrationlintreport.FailOnNone,
		Changed:   migrationlintreport.ChangedOptions{Dialect: true},
	}, cfg)

	c.Assert(err, qt.ErrorMatches, `migrations directory : .*`)
}

func TestBuild_ExplicitZeroProjectLatestReachesValidation(t *testing.T) {
	c := qt.New(t)
	cfg, err := projectconfig.ParsePtah([]byte(`
lint:
  latest: 0
`), "ptah.yaml", "")
	c.Assert(err, qt.IsNil)

	_, err = migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:       t.TempDir(),
		FS:        fstest.MapFS{"1_init.sql": {Data: []byte("CREATE TABLE users (id int);")}},
		DirFormat: string(migrator.MigrationDirFormatAtlas),
		Dialect:   "sqlite",
		FailOn:    migrationlintreport.FailOnNone,
		Changed:   migrationlintreport.ChangedOptions{Dialect: true},
	}, cfg)

	c.Assert(err, qt.ErrorMatches, `--latest must be greater than zero`)
}

func TestBuild_ExplicitGitBaseSuppressesProjectLatest(t *testing.T) {
	c := qt.New(t)
	latest := 1
	cfg := projectconfig.Config{
		Lint: projectconfig.LintConfig{Latest: &latest},
	}

	_, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:       t.TempDir(),
		FS:        fstest.MapFS{"1_init.sql": {Data: []byte("CREATE TABLE users (id int);")}},
		DirFormat: string(migrator.MigrationDirFormatAtlas),
		Dialect:   "sqlite",
		GitBase:   "-unsafe",
		FailOn:    migrationlintreport.FailOnNone,
		Changed: migrationlintreport.ChangedOptions{
			Dialect: true,
			GitBase: true,
		},
	}, cfg)

	c.Assert(err, qt.ErrorMatches, `--git-base "-unsafe" is not a safe Git ref`)
}

func TestBuild_ExplicitLatestSuppressesProjectGitSelector(t *testing.T) {
	c := qt.New(t)
	cfg := projectconfig.Config{
		Lint: projectconfig.LintConfig{
			GitBase: "-unsafe",
			GitDir:  "/not/a/repository",
		},
	}

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:       t.TempDir(),
		FS:        fstest.MapFS{"1_init.sql": {Data: []byte("CREATE TABLE users (id int);")}},
		DirFormat: string(migrator.MigrationDirFormatAtlas),
		Dialect:   "sqlite",
		Latest:    1,
		FailOn:    migrationlintreport.FailOnNone,
		Changed: migrationlintreport.ChangedOptions{
			Dialect: true,
			Latest:  true,
		},
	}, cfg)

	c.Assert(err, qt.IsNil)
	c.Assert(report.Versions, qt.DeepEquals, []int64{1})
}

func TestBuild_LatestAndAnalysisShareOneSourceSnapshot(t *testing.T) {
	c := qt.New(t)
	source := &countingSnapshotSource{
		FS: fstest.MapFS{
			"1_old.sql": {Data: []byte("DROP TABLE old_data;\n")},
			"2_new.sql": {Data: []byte("DROP TABLE new_data;\n")},
		},
		reads: map[string]int{},
	}

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:       t.TempDir(),
		FS:        source,
		DirFormat: string(migrator.MigrationDirFormatAtlas),
		FailOn:    migrationlintreport.FailOnNone,
		Latest:    1,
		Changed:   migrationlintreport.ChangedOptions{Latest: true},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Versions, qt.DeepEquals, []int64{2})
	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].File, qt.Contains, "2_new.sql")
	c.Assert(source.reads, qt.DeepEquals, map[string]int{
		"1_old.sql": 1,
		"2_new.sql": 1,
	})
}

func TestBuild_ProvidedSnapshotDoesNotRequireSourceDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeLintTestFile(c, dir, "1_drop.sql", "DROP TABLE users;\n")
	snapshot, err := migrationsnapshot.Capture(os.DirFS(dir))
	c.Assert(err, qt.IsNil)
	c.Assert(os.RemoveAll(dir), qt.IsNil)

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:       dir,
		FS:        snapshot,
		DirFormat: string(migrator.MigrationDirFormatAtlas),
		FailOn:    migrationlintreport.FailOnNone,
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].Rule, qt.Equals, "DS101")
}

func TestBuild_LoadsConventionalLintConfigFromSnapshot(t *testing.T) {
	c := qt.New(t)
	source := &countingSnapshotSource{
		FS: fstest.MapFS{
			".ptah-lint.yaml": {Data: []byte("disabled-rules: [DS101]\n")},
			"1_drop.sql":      {Data: []byte("DROP TABLE users;\n")},
		},
		reads: map[string]int{},
	}

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:       t.TempDir(),
		FS:        source,
		DirFormat: string(migrator.MigrationDirFormatAtlas),
		FailOn:    migrationlintreport.FailOnNone,
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.DisabledRules, qt.DeepEquals, []string{"DS101"})
	c.Assert(report.Findings, qt.HasLen, 0)
	c.Assert(source.reads, qt.DeepEquals, map[string]int{
		".ptah-lint.yaml": 1,
		"1_drop.sql":      1,
	})
}

func TestBuild_FailOnErrorDoesNotFailWarnings(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeWarningMigration(c, dir)

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:       dir,
		DirFormat: string(migrator.MigrationDirFormatPtah),
		Dialect:   "postgres",
		FailOn:    migrationlintreport.FailOnError,
		Changed:   migrationlintreport.ChangedOptions{Dialect: true},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed, qt.IsFalse)
	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].Rule, qt.Equals, "PG101")
}

func TestBuild_FailOnAnyFailsWarnings(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeWarningMigration(c, dir)

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:       dir,
		DirFormat: string(migrator.MigrationDirFormatPtah),
		Dialect:   "postgres",
		FailOn:    migrationlintreport.FailOnAny,
		Changed:   migrationlintreport.ChangedOptions{Dialect: true},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed, qt.IsTrue)
	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].Rule, qt.Equals, "PG101")
}

func TestWrite_GitHubActionsEscapesWorkflowCommandCharacters(t *testing.T) {
	c := qt.New(t)

	var buf bytes.Buffer
	err := migrationlintreport.Write(&buf, migrationlintreport.FormatGitHubActions, migrationlintreport.Report{
		Findings: []migrationlint.Finding{{
			Rule:     "DS101",
			Severity: migrationlint.SeverityError,
			File:     "dir/evil,file::name.sql",
			Line:     3,
			Message:  "50% data loss\r\nsecond line",
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(buf.String(), qt.Contains, "::error file=dir/evil%2Cfile%3A%3Aname.sql,line=3::")
	c.Assert(buf.String(), qt.Contains, "DS101: 50%25 data loss%0D%0Asecond line")
	c.Assert(buf.String(), qt.Not(qt.Contains), "evil,file::name")
}

type countingSnapshotSource struct {
	fs.FS
	reads map[string]int
}

func (f *countingSnapshotSource) ReadFile(name string) ([]byte, error) {
	f.reads[name]++
	return fs.ReadFile(f.FS, name)
}

func TestWrite_GitHubActionsEscapesErrorReport(t *testing.T) {
	c := qt.New(t)

	var buf bytes.Buffer
	err := migrationlintreport.Write(&buf, migrationlintreport.FormatGitHubActions, migrationlintreport.ErrorReport(
		migrationlintreport.FailOnError,
		"bad\nnews: 100%",
	))

	c.Assert(err, qt.IsNil)
	c.Assert(buf.String(), qt.Equals, "::error::bad%0Anews: 100%25\n")
}
