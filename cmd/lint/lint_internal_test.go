package lint

// White-box testing required: these tests verify the command's pre-resolution
// precedence before an OCI migration source is fetched.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/internal/migrationlintreport"
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
