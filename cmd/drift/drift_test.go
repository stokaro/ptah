package drift_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/drift"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
)

func TestNewDriftCommand_Creation(t *testing.T) {
	c := qt.New(t)

	cmd := drift.NewDriftCommand()

	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "drift")
	c.Assert(cmd.Short, qt.Contains, "drift")
}

func TestNewDriftCommand_ExposesRepeatableSchemaSources(t *testing.T) {
	c := qt.New(t)

	cmd := drift.NewDriftCommand()

	rootDir := cmd.Flags().Lookup("root-dir")
	c.Assert(rootDir, qt.IsNotNil)
	c.Assert(rootDir.Value.Type(), qt.Equals, "stringArray")

	schemaFile := cmd.Flags().Lookup("schema-file")
	c.Assert(schemaFile, qt.IsNotNil)
	c.Assert(schemaFile.Value.Type(), qt.Equals, "stringArray")

	c.Assert(cmd.Flags().Lookup("schema-cmd"), qt.IsNotNil)
	c.Assert(cmd.Flags().Lookup("schema-format"), qt.IsNotNil)
	c.Assert(cmd.Flags().Lookup("plain-http"), qt.IsNotNil)
}

func TestRunDrift_MissingDatabaseURLReturnsCode2(t *testing.T) {
	c := qt.New(t)

	cmd := drift.NewDriftCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetArgs([]string{"--root-dir", "."})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr.String(), qt.Contains, "database URL is required")
}
