package schemapush_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/schemapush"
)

func TestNewSchemaPushCommand_Help(t *testing.T) {
	c := qt.New(t)
	cmd := schemapush.NewSchemaPushCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(output.String(), qt.Contains, "push <oci-reference>")
	c.Assert(output.String(), qt.Contains, "--root-dir")
	c.Assert(output.String(), qt.Contains, "--schema-file")
	c.Assert(output.String(), qt.Contains, "--plain-http")
}

func TestNewSchemaPushCommand_RejectsMissingReference(t *testing.T) {
	c := qt.New(t)
	cmd := schemapush.NewSchemaPushCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs(nil)

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "expected exactly 1 positional argument\\(s\\), got 0")
	c.Assert(stderr.String(), qt.Contains, "expected exactly 1 positional argument(s), got 0")
}
