package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
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
		{name: "positive selection", scope: atlasfilter.Scope{Include: []string{"kept"}, DefaultSchema: "public"}},
		{name: "exclusion only", scope: atlasfilter.Scope{Exclude: []string{"dropped"}, DefaultSchema: "public"}},
		{name: "schema universe", scope: atlasfilter.Scope{Schemas: []string{"public"}, DefaultSchema: "public"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			schema := &catalog.Database{
				Tables: []catalog.Table{
					{Name: "kept", Schema: "public"},
					{Name: "dropped", Schema: "public"},
				},
				NotDescribed: coverage.Set{}.With(coverage.Refused(coverage.Extension)),
			}

			got, err := atlasfilter.ScopeDatabase(schema, test.scope)

			c.Assert(err, qt.IsNil)
			c.Assert(got.NotDescribed, qt.DeepEquals,
				coverage.Set{}.With(coverage.Refused(coverage.Extension)))
		})
	}
}

func TestScopeGeneratedKeepsCoverage(t *testing.T) {
	tests := []struct {
		name  string
		scope atlasfilter.Scope
	}{
		{name: "positive selection", scope: atlasfilter.Scope{Include: []string{"kept"}, DefaultSchema: "public"}},
		{name: "exclusion only", scope: atlasfilter.Scope{Exclude: []string{"dropped"}, DefaultSchema: "public"}},
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
