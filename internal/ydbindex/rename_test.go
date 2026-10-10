package ydbindex_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbindex"
)

// TestRenameKeeps carries an index's partitioning through a rename, which the
// owner compares under the new name, and nothing a rename cannot carry, such
// as a vector index's settings, alone or beside the partitioning.
func TestRenameKeeps(t *testing.T) {
	partitioning := &ydbschema.DesiredIndexPartitioning{IndexPartitioning: ydbschema.IndexPartitioning{MinPartitions: 3}}
	vector := &ydbschema.DesiredVectorIndex{}
	tests := []struct {
		name   string
		facets schemaext.Facets
		want   bool
	}{
		{name: "no facet", facets: schemaext.Facets{}, want: true},
		{name: "partitioning", facets: must.Must(schemaext.NewFacets(partitioning)), want: true},
		{name: "vector settings", facets: must.Must(schemaext.NewFacets(vector)), want: false},
		{name: "vector settings beside partitioning", facets: must.Must(schemaext.NewFacets(partitioning, vector)), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbindex.RenameKeeps(test.facets), qt.Equals, test.want)
		})
	}
}
