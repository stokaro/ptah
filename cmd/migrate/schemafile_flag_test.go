package migrate_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/migrate"
)

func TestMigratePlanCommandExposesRepeatableSchemaFileFlag(t *testing.T) {
	c := qt.New(t)

	cmd := migrate.NewMigrateCommand()

	schemaFile := cmd.Flags().Lookup("schema-file")
	c.Assert(schemaFile, qt.IsNotNil)
	c.Assert(schemaFile.Value.Type(), qt.Equals, "stringArray")
}

func TestMigrateGenerateCommandExposesRepeatableSchemaFileFlag(t *testing.T) {
	c := qt.New(t)

	cmd := migrate.NewMigrateGenerateCommand()

	schemaFile := cmd.Flags().Lookup("schema-file")
	c.Assert(schemaFile, qt.IsNotNil)
	c.Assert(schemaFile.Value.Type(), qt.Equals, "stringArray")
}
