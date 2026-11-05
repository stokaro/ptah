package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/internal/atlasfilter"
)

// Filtering narrows what a description CONTAINS. It must never widen what the
// description CLAIMS to cover, and it must not flatten the reason the claim
// carries either -- a projection that dropped `reason=not-inspected` would leave
// a record that still blocks the removal but can no longer say why
// (stokaro/ptah#1346): a projection that dropped the coverage record
// would turn a document's declared silence back into desired absence one
// function call after it was recorded, which is stokaro/ptah#1276 reappearing
// inside its own fix.
//
// These two tests exist because the database projection is a field-by-field
// constructor. That is the shape a new field is dropped from in silence, and no
// other test in this package would notice.

func TestScopeDatabaseKeepsCoverage(t *testing.T) {
	tests := []struct {
		name  string
		scope atlasfilter.Scope
	}{
		{name: "positive selection", scope: atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"kept"}, DefaultSchema: "public"}},
		{name: "exclusion only", scope: atlasfilter.Scope{Selections: ownerSelections(), Exclude: []string{"dropped"}, DefaultSchema: "public"}},
		{name: "schema universe", scope: atlasfilter.Scope{Selections: ownerSelections(), Schemas: []string{"public"}, DefaultSchema: "public"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			context := planningContextCoverage(c)
			// The context sits in a schema the filter leaves out, which is where a
			// filtered read puts it.
			contextObjects := must.Must(schemaext.NewObjects(tsschema.ObservedContinuousAggregateObject("outside", "hourly",
				tsschema.ObservedContinuousAggregate{Definition: "SELECT 1", HypertableSchema: "outside", HypertableName: "readings"})))
			schema := &catalog.Database{
				Tables: []catalog.Table{
					{Name: "kept", Schema: "public"},
					{Name: "dropped", Schema: "public"},
				},
				NotDescribed:            coverage.Set{}.With(coverage.Refused(coverage.Extension)),
				DatabasePath:            "/local",
				PlanningContextObjects:  contextObjects,
				PlanningContextCoverage: context,
				// No selection keeps the table, and the filter keeps the record.
				UnreadTables: []catalog.UnreadTable{{Identity: objectidentity.NewBuilder(identifier.ForDialect("postgres")).TablePartsVerbatim("public", "rep"),
					Reason: "rep is a replica table"}},
			}

			got, err := atlasfilter.ScopeDatabase(schema, test.scope)

			c.Assert(err, qt.IsNil)
			c.Assert(got.NotDescribed, qt.DeepEquals,
				coverage.Set{}.With(coverage.Refused(coverage.Extension)))
			c.Assert(got.DatabasePath, qt.Equals, "/local")
			c.Assert(got.PlanningContextObjects.Equal(contextObjects), qt.IsTrue)
			c.Assert(got.PlanningContextCoverage.Equal(context), qt.IsTrue)
			c.Assert(got.UnreadTables, qt.DeepEquals, schema.UnreadTables)
		})
	}
}

// planningContextCoverage is what a filtered read established about the
// coordination nodes of the whole database, as its planning context.
func planningContextCoverage(c *qt.C) schemaext.Coverage {
	c.Helper()
	model := schemaext.CodecIdentity{Owner: "example.com/test", Kind: "example.com/test/node", Representation: schemaext.Observed, Version: 1, Definition: "node"}
	domain, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Complete})
	c.Assert(err, qt.IsNil)
	known, err := schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{Model: model, Knowledge: domain}}, nil)
	c.Assert(err, qt.IsNil)
	return known
}

func TestScopeGeneratedKeepsCoverage(t *testing.T) {
	tests := []struct {
		name  string
		scope atlasfilter.Scope
	}{
		{name: "positive selection", scope: atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"kept"}, DefaultSchema: "public"}},
		{name: "exclusion only", scope: atlasfilter.Scope{Selections: ownerSelections(), Exclude: []string{"dropped"}, DefaultSchema: "public"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			schema := &schemamodel.Database{
				Tables: []schemamodel.Table{
					{Name: "kept", StructName: "Kept", Schema: "public"},
					{Name: "dropped", StructName: "Dropped", Schema: "public"},
				},
				NotDescribed: coverage.Set{}.With(coverage.Object{
					Kind:       coverage.Sequence,
					Reason:     coverage.SuppressedByPolicy,
					Provenance: coverage.Defaulted,
				}),
			}

			got, err := atlasfilter.ScopeGenerated(schema, test.scope)

			c.Assert(err, qt.IsNil)
			c.Assert(got.NotDescribed, qt.DeepEquals, coverage.Set{}.With(coverage.Object{
				Kind:       coverage.Sequence,
				Reason:     coverage.SuppressedByPolicy,
				Provenance: coverage.Defaulted,
			}))
		})
	}
}
