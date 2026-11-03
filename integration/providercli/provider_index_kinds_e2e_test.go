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

// kindDeclarations are the column declarations of records with index
// records_value on value, written with type declared.
func kindDeclarations(declared string) *schemamodel.Database {
	desired := columnDeclarations()
	desired.Indexes = []schemamodel.Index{{Name: "records_value", StructName: "Record", Fields: []string{"value"}, Type: declared}}
	return desired
}

// kindObservation is the observation of records holding records_value with
// the kind the fixture reports as method.
func kindObservation(method string) *catalog.Database {
	current := columnObservation()
	current.Indexes = []catalog.Index{{Name: "records_value", TableName: "records", Columns: []string{"value"}, Method: method}}
	return current
}

// The fixture prepares the kind of each declared index, so a change of kind
// alone drops the held index and creates the declared one under its name,
// and once the database holds the declared kind the next comparison is
// synced, whichever spelling the source uses. The created index carries the
// type the source wrote.
func TestProviderIndexKindChangeReplacesTheIndex(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "column-semantics")
	replaced := []difftypes.IndexRef{{Name: "records_value", TableName: "records"}}

	diff, err := schemadiff.CompareWithDialect(c.Context(), kindDeclarations("ASYNC"), kindObservation("global"), "fixture", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.IndexesRemoved, qt.DeepEquals, replaced)
	c.Assert(diff.IndexesAdded.Refs(), qt.DeepEquals, replaced)
	c.Assert(diff.IndexesAdded[0].Index.Type, qt.Equals, "ASYNC")

	for _, declared := range []string{"ASYNC", "global async", "async"} {
		t.Run("synced as "+declared, func(t *testing.T) {
			c := qt.New(t)
			diff, err := schemadiff.CompareWithDialect(c.Context(), kindDeclarations(declared), kindObservation("async"), "fixture", runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}
}

// A declared kind the fixture cannot read refuses the comparison rather than
// compare the index without its kind. The host does not repeat a provider's
// error text, which may hold secrets; schema validation is where a provider
// gives its reason.
func TestProviderIndexKindRefusesAnUnknownKind(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "column-semantics")
	diff, err := schemadiff.CompareWithDialect(c.Context(), kindDeclarations("HASH"), kindObservation("global"), "fixture", runtime)
	c.Assert(err, qt.ErrorMatches, `.*provider prepare tables failed \(InvalidArgument\)`)
	c.Assert(diff, qt.IsNil)
}

// Without prepared kinds the host compares no kind, and a change of kind
// alone plans nothing.
func TestProviderIndexKindIsNotComparedWithoutPreparedKinds(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "column-semantics", []string{"FIXTURE_INDEX_METHODS=off"})
	diff, err := schemadiff.CompareWithDialect(c.Context(), kindDeclarations("ASYNC"), kindObservation("global"), "fixture", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}
