//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// A provider that negotiates schema.observed-index-facets reports the settings
// of an index it holds as a facet the index owns, and the host reads it into
// the index of its catalog, with the coverage of that index, so the comparison
// has the index's side of a facet a source declares on the index.
func TestProviderInspectionReadsIndexFacets_HappyPath(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "column-semantics")
	connection := observedConnection(c, host, "index_facet")
	snapshot, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(snapshot.Indexes, qt.HasLen, 1)
	kind := schemaext.Kind("example.org/fixture/retention")
	c.Assert(snapshot.Indexes[0].Facets.Kinds(), qt.DeepEquals, []schemaext.Kind{kind})
	subject := objectidentity.NewBuilder(comparisonRules()).IndexParts(snapshot.Tables[0].Schema, "index_facet", "index_facet_id")
	c.Assert(snapshot.FeatureCoverage.Lookup(kind, subject).State, qt.Equals, schemaext.Complete)
	// The table and its column carry no facet of their own.
	c.Assert(snapshot.Tables[0].Facets.Kinds(), qt.HasLen, 0)
}

// A read that carries an index facet the host did not negotiate is refused
// whole, rather than the facet attached to the table or dropped. The
// provider's own SDK refuses its reply before it is sent, so the host reports
// the failed call.
func TestProviderInspectionReadsIndexFacets_FailurePath(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderEnvironment(c, "column-semantics", []string{"FIXTURE_INDEX_FACETS=off"})
	connection := observedConnection(c, host, "index_facet")
	snapshot, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.ErrorMatches, `provider inspect failed \(FailedPrecondition\)`)
	c.Assert(snapshot, qt.IsNil)
}
