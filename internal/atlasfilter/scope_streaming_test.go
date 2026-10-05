package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasfilter"
)

func TestScope_StreamingQueriesAndTheirPools(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{StreamingQueries: []schemamodel.StreamingQuery{
		{Name: "copy", Spec: ast.StreamingQuerySpec{ResourcePool: "batch"}}, {Name: "other"},
	}, ResourcePools: []schemamodel.ResourcePool{{Name: "batch"}, {Name: "idle"}}}
	held := &catalog.Database{StreamingQueries: []catalog.StreamingQuery{
		{Name: "copy", Spec: ast.StreamingQuerySpec{ResourcePool: "batch"}}, {Name: "other"},
	}, ResourcePools: []catalog.ResourcePool{{Name: "batch"}, {Name: "idle"}}}
	scope := atlasfilter.Scope{Include: []string{"copy[type=streaming_query]"}}
	generated, _, err := atlasfilter.ScopeGeneratedSelectionReport(declared, scope)
	c.Assert(err, qt.IsNil)
	live, err := atlasfilter.ScopeDatabase(held, scope)
	c.Assert(err, qt.IsNil)
	c.Assert(generated.StreamingQueries, qt.DeepEquals, declared.StreamingQueries[:1])
	c.Assert(live.StreamingQueries, qt.DeepEquals, held.StreamingQueries[:1])
	c.Assert(generated.ResourcePools, qt.DeepEquals, declared.ResourcePools[:1])
	c.Assert(live.ResourcePools, qt.DeepEquals, held.ResourcePools[:1])
	scope = atlasfilter.Scope{Exclude: []string{"copy[type=streaming_query]"}}
	generated, _, err = atlasfilter.ScopeGeneratedSelectionReport(declared, scope)
	c.Assert(err, qt.IsNil)
	live, err = atlasfilter.ScopeDatabase(held, scope)
	c.Assert(err, qt.IsNil)
	c.Assert(generated.StreamingQueries, qt.DeepEquals, declared.StreamingQueries[1:])
	c.Assert(live.StreamingQueries, qt.DeepEquals, held.StreamingQueries[1:])
}
