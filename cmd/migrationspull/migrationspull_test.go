package migrationspull_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/migrationspull"
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
	c.Assert(output.String(), qt.Contains, "pull <oci-reference>")
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
