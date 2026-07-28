package schemapull_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/schemapull"
)

func TestNewSchemaPullCommand_Help(t *testing.T) {
	c := qt.New(t)
	cmd := schemapull.NewSchemaPullCommand()
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

func TestNewSchemaPullCommand_RequiresOutput(t *testing.T) {
	c := qt.New(t)
	cmd := schemapull.NewSchemaPullCommand()
	cmd.SetArgs([]string{"oci://registry.example/acme/schema"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "schema artifact output file is required")
}
