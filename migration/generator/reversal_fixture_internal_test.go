package generator

// White-box testing required: fixtures exercise reversal before SQL planning so the structural
// coverage gate can distinguish a missing change from an omitted statement.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/migration/schemadiff/difftypes"
)

func reverseForTest(t testing.TB, diff *difftypes.SchemaDiff, desired *schemamodel.Database, current *catalog.Database, dialect string) *difftypes.SchemaDiff {
	t.Helper()
	return reverseWithRuntimeForTest(t, builtintest.Runtime(), diff, desired, current, dialect)
}

func reverseWithRuntimeForTest(t testing.TB, runtime Runtime, diff *difftypes.SchemaDiff, desired *schemamodel.Database, current *catalog.Database, dialect string) *difftypes.SchemaDiff {
	t.Helper()
	c := qt.New(t)
	prior, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), current, dialect, runtime)
	c.Assert(err, qt.IsNil)
	reversed := reverseSchemaDiffWithPrior(diff, desired, current, prior, dialect)
	_, err = reverseFeatureChanges(t.Context(), diff, reversed, dialect, capability.ForDialect(dialect), runtime)
	c.Assert(err, qt.IsNil)
	return reversed
}
