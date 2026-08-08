package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

// Filtering narrows what a description CONTAINS. It must never widen what the
// description CLAIMS to cover: a projection that dropped the coverage record
// would turn a document's declared silence back into desired absence one
// function call after it was recorded, which is stokaro/ptah#1276 reappearing
// inside its own fix.
//
// These two tests exist because the database projection is a field-by-field
// constructor. That is the shape a new field is dropped from in silence, and no
// other test in this package would notice.

func TestScopeDatabaseKeepsCoverage(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		scope atlasfilter.Scope
	}{
		{name: "positive selection", scope: atlasfilter.Scope{Include: []string{"kept"}, DefaultSchema: "public"}},
		{name: "exclusion only", scope: atlasfilter.Scope{Exclude: []string{"dropped"}, DefaultSchema: "public"}},
		{name: "schema universe", scope: atlasfilter.Scope{Schemas: []string{"public"}, DefaultSchema: "public"}},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			schema := &dbschematypes.DBSchema{
				Tables: []dbschematypes.DBTable{
					{Name: "kept", Schema: "public"},
					{Name: "dropped", Schema: "public"},
				},
				NotDescribed: coverage.Set{}.WithKind(coverage.Extension),
			}

			got, err := atlasfilter.ScopeDatabase(schema, test.scope)

			c.Assert(err, qt.IsNil)
			c.Assert(got.NotDescribed, qt.DeepEquals, coverage.Set{}.WithKind(coverage.Extension))
		})
	}
}

func TestScopeGeneratedKeepsCoverage(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		scope atlasfilter.Scope
	}{
		{name: "positive selection", scope: atlasfilter.Scope{Include: []string{"kept"}, DefaultSchema: "public"}},
		{name: "exclusion only", scope: atlasfilter.Scope{Exclude: []string{"dropped"}, DefaultSchema: "public"}},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			schema := &goschema.Database{
				Tables: []goschema.Table{
					{Name: "kept", StructName: "Kept", Schema: "public"},
					{Name: "dropped", StructName: "Dropped", Schema: "public"},
				},
				NotDescribed: coverage.Set{}.WithKind(coverage.Sequence),
			}

			got, err := atlasfilter.ScopeGenerated(schema, test.scope)

			c.Assert(err, qt.IsNil)
			c.Assert(got.NotDescribed, qt.DeepEquals, coverage.Set{}.WithKind(coverage.Sequence))
		})
	}
}
