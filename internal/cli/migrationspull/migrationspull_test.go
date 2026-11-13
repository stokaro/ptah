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
func TestNewMigrationsPullCommand_RefusesPlainHTTPOnAGitOrHTTPSReference(t *testing.T) {
	for _, reference := range []string{"git+https://git.invalid/acme/app.git", "https://artifacts.invalid/acme/migrations.tar.gz"} {
		t.Run(reference, func(t *testing.T) {
			c := qt.New(t)
			cmd := migrationspull.NewMigrationsPullCommand()
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			cmd.SetArgs([]string{reference, "--path", "db/migrations", "--out", filepath.Join(c.TempDir(), "out"), "--plain-http"})

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, `--plain-http is for an oci:// reference`)
		})
	}
}
