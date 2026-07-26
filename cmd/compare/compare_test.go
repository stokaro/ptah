package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/compare"
)

func TestCompareCommandExposesRepeatableSchemaFileFlag(t *testing.T) {
	c := qt.New(t)

	cmd := compare.NewCompareCommand()

	schemaFile := cmd.Flags().Lookup("schema-file")
	c.Assert(schemaFile, qt.IsNotNil)
	c.Assert(schemaFile.Value.Type(), qt.Equals, "stringArray")

	rootDir := cmd.Flags().Lookup("root-dir")
	c.Assert(rootDir, qt.IsNotNil)
	c.Assert(rootDir.Value.Type(), qt.Equals, "stringArray")
}
