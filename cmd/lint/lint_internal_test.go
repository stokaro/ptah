package lint

// White-box testing required: these tests verify the command's pre-resolution
// precedence and immutable snapshot handoff before report construction.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/migrationlintreport"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestPrepareReportOptions_PresentEmptyDirSuppressesDefault(t *testing.T) {
	c := qt.New(t)
	cfg, err := projectconfig.ParsePtah([]byte("migration:\n  dir: \"\"\n"), "ptah.yaml", "")
	c.Assert(err, qt.IsNil)
	cmd := NewLintCommand()

	got, provenance, err := prepareReportOptions(
		cmd,
		migrationlintreport.Options{Dir: "oci://"},
		cfg,
		sourceOptions{attach: true},
	)

	c.Assert(err, qt.ErrorMatches, "--attach requires an OCI migration source")
	c.Assert(got, qt.DeepEquals, migrationlintreport.Options{})
	c.Assert(provenance, qt.IsNil)
}

func TestPrepareReportOptions_CapturesLocalDirectoryBeforeBuild(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	c.Assert(
		os.WriteFile(
			filepath.Join(dir, "1_drop.sql"),
			[]byte("DROP TABLE original;\n"),
			0o600,
		),
		qt.IsNil,
	)
	cmd := NewLintCommand()
	c.Assert(cmd.Flags().Set("dir", dir), qt.IsNil)

	got, provenance, err := prepareReportOptions(
		cmd,
		migrationlintreport.Options{
			Dir:       dir,
			DirFormat: string(migrator.MigrationDirFormatAtlas),
			FailOn:    migrationlintreport.FailOnNone,
			Changed: migrationlintreport.ChangedOptions{
				Dir:       true,
				DirFormat: true,
			},
		},
		projectconfig.Config{},
		sourceOptions{},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(provenance, qt.IsNil)

	c.Assert(os.Rename(dir, filepath.Join(root, "captured")), qt.IsNil)
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	c.Assert(
		os.WriteFile(
			filepath.Join(dir, "1_create.sql"),
			[]byte("CREATE TABLE replacement (id INTEGER);\n"),
			0o600,
		),
		qt.IsNil,
	)

	report, err := migrationlintreport.Build(c.Context(), got, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].Rule, qt.Equals, "DS101")
	c.Assert(report.Findings[0].Context.Subjects, qt.HasLen, 1)
	c.Assert(report.Findings[0].Context.Subjects[0].Name, qt.Equals, "original")
}

func TestPrepareReportOptions_ExplicitDirWinsPresentEmptyConfig(t *testing.T) {
	c := qt.New(t)
	cfg, err := projectconfig.ParsePtah([]byte("migration:\n  dir: \"\"\n"), "ptah.yaml", "")
	c.Assert(err, qt.IsNil)
	cmd := NewLintCommand()
	c.Assert(cmd.Flags().Set("dir", "oci://"), qt.IsNil)

	got, provenance, err := prepareReportOptions(
		cmd,
		migrationlintreport.Options{Dir: "oci://"},
		cfg,
		sourceOptions{attach: true},
	)

	c.Assert(err, qt.ErrorMatches, "invalid OCI reference: invalid reference: missing registry or repository")
	c.Assert(got, qt.DeepEquals, migrationlintreport.Options{})
	c.Assert(provenance, qt.IsNil)
}
