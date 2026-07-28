package migrationspush_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/migrationspush"
)

func TestNewMigrationsPushCommand_Help(t *testing.T) {
	c := qt.New(t)
	cmd := migrationspush.NewMigrationsPushCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(output.String(), qt.Contains, "push <oci-reference>")
	c.Assert(output.String(), qt.Contains, "--migrations-dir")
	c.Assert(output.String(), qt.Contains, "--plain-http")
	c.Assert(output.String(), qt.Contains, "--verify-sum")
}

func TestNewMigrationsPushCommand_RejectsMissingReference(t *testing.T) {
	c := qt.New(t)
	cmd := migrationspush.NewMigrationsPushCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs(nil)

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "expected exactly 1 positional argument\\(s\\), got 0")
	c.Assert(stderr.String(), qt.Contains, "expected exactly 1 positional argument(s), got 0")
}

func TestNewMigrationsPushCommand_RejectsUnknownDirectoryFormat(t *testing.T) {
	c := qt.New(t)
	cmd := migrationspush.NewMigrationsPushCommand()
	cmd.SetArgs([]string{
		"oci://registry.example/acme/migrations",
		"--dir-format",
		"unknown",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unknown migration directory format "unknown": expected auto, ptah, or atlas`)
}
