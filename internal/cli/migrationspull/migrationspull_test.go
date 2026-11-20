package migrationspull_test

import (
	"bytes"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migrationspull"
)

func TestNewMigrationsPullCommand_Help(t *testing.T) {
	c := qt.New(t)
	cmd := migrationspull.NewMigrationsPullCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(output.String(), qt.Contains, "pull <oci-reference | git-reference | https-url>")
	c.Assert(output.String(), qt.Contains, "--out")
	c.Assert(output.String(), qt.Contains, "--plain-http")
}

func TestNewMigrationsPullCommand_RejectsMissingReference(t *testing.T) {
	c := qt.New(t)
	cmd := migrationspull.NewMigrationsPullCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs(nil)

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "expected exactly 1 positional argument\\(s\\), got 0")
	c.Assert(stderr.String(), qt.Contains, "expected exactly 1 positional argument(s), got 0")
}

func TestNewMigrationsPullCommand_RequiresOutputDirectory(t *testing.T) {
	c := qt.New(t)
	cmd := migrationspull.NewMigrationsPullCommand()
	cmd.SetArgs([]string{"oci://registry.example/acme/migrations"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "migration artifact output directory is required")
}

// A Git or an HTTPS pull reaches its server over its own transport, so
// --plain-http would claim something the command does not do. Each refusal comes
// before any network access: the address does not resolve.
func TestNewMigrationsPullCommand_RefusesAnOCIFlagOnAGitOrHTTPSReference(t *testing.T) {
	tests := []struct {
		reference string
		flag      string
	}{
		{reference: "git+https://git.invalid/acme/app.git", flag: "--plain-http"},
		{reference: "https://artifacts.invalid/acme/migrations.tar.gz", flag: "--plain-http"},
		{reference: "git+https://git.invalid/acme/app.git", flag: "--skip-validation"},
		{reference: "https://artifacts.invalid/acme/migrations.tar.gz", flag: "--skip-validation"},
	}
	for _, test := range tests {
		t.Run(test.flag+" "+test.reference, func(t *testing.T) {
			c := qt.New(t)
			cmd := migrationspull.NewMigrationsPullCommand()
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			cmd.SetArgs([]string{test.reference, "--path", "db/migrations", "--out", filepath.Join(c.TempDir(), "out"), test.flag})

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, test.flag+` is for an oci:// reference`)
		})
	}
}
