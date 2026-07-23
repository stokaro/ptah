package migrationlintreport_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/internal/migrationlintreport"
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
