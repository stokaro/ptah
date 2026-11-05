//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// coveredDeclarations are the column declarations of records with index
// records_value on value, covering the columns given.
func coveredDeclarations(covered ...string) *schemamodel.Database {
	desired := columnDeclarations()
	desired.Indexes = []schemamodel.Index{{Name: "records_value", StructName: "Record", Fields: []string{"value"}, IncludeColumns: covered}}
	return desired
}

// coveredObservation is the observation of records holding records_value,
// covering the columns given.
func coveredObservation(covered ...string) *catalog.Database {
	current := columnObservation()
	current.Indexes = []catalog.Index{{Name: "records_value", TableName: "records", Columns: []string{"value"}, IncludeColumns: covered}}
	return current
}

// The fixture states no answer for index_covering_columns. An index whose
// covered columns the database holds as declared compares clean, as YDB's
// COVER columns compared while the built-in engine's preset answered the key;
// read as absent, the key planned the index again on every run.
func TestProviderEngineComparesACoveredIndex_HappyPath(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "column-semantics", []string{"FIXTURE_INDEX_METHODS=off"})

	diff, err := schemadiff.CompareWithDialect(c.Context(), coveredDeclarations("id"), coveredObservation("id"), "fixture", runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

// A covered column the database does not hold replaces the index.
func TestProviderEngineComparesACoveredIndex_FailurePath(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "column-semantics", []string{"FIXTURE_INDEX_METHODS=off"})
	replaced := []difftypes.IndexRef{{Name: "records_value", TableName: "records"}}

	diff, err := schemadiff.CompareWithDialect(c.Context(), coveredDeclarations("id"), coveredObservation(), "fixture", runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.IndexesRemoved, qt.DeepEquals, replaced)
	c.Assert(diff.IndexesAdded.Refs(), qt.DeepEquals, replaced)
}
