package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_ResourcePool_RoundTrip writes YDB resource pools and classifiers
// as annotations that parse back to the same declarations, so `ptah
// introspect` of a YDB database keeps them: a fraction stays a fraction, and
// a pool with no setting stays one.
func TestRender_ResourcePool_RoundTrip(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		ResourcePools: []schemamodel.ResourcePool{
			{Name: "idle"},
			{Name: "batch", Spec: ast.ResourcePoolSpec{
				ConcurrentQueryLimit: new(int32(10)), QueueSize: new(int32(0)),
				DatabaseLoadCPUThreshold: new(80.5), QueryMemoryLimitPercentPerNode: new(25.0),
				QueryCPULimitPercentPerNode: new(0.001), TotalCPULimitPercentPerNode: new(100.0),
				ResourceWeight: new(2.25),
			}},
		},
		ResourcePoolClassifiers: []schemamodel.ResourcePoolClassifier{
			{Name: "everyone", Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 0}},
			{Name: "etl", Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "batch", MemberName: "etl", Rank: 10}},
		},
	}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{PackageName: "models", SingleFile: true, Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	parsed, err := goschema.ParseSource(files[0].Name, files[0].Data)

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.ResourcePools, qt.HasLen, 2)
	c.Assert([]any{parsed.ResourcePools[0].Name, parsed.ResourcePools[0].Spec}, qt.DeepEquals,
		[]any{"batch", db.ResourcePools[1].Spec})
	c.Assert([]any{parsed.ResourcePools[1].Name, parsed.ResourcePools[1].Spec}, qt.DeepEquals,
		[]any{"idle", ast.ResourcePoolSpec{}})
	c.Assert(parsed.ResourcePoolClassifiers, qt.HasLen, 2)
	c.Assert([]any{parsed.ResourcePoolClassifiers[0].Name, parsed.ResourcePoolClassifiers[0].Spec}, qt.DeepEquals,
		[]any{"etl", db.ResourcePoolClassifiers[1].Spec})
	c.Assert([]any{parsed.ResourcePoolClassifiers[1].Name, parsed.ResourcePoolClassifiers[1].Spec}, qt.DeepEquals,
		[]any{"everyone", db.ResourcePoolClassifiers[0].Spec})
}

// A schema whose only global objects are pools, or only classifiers, still
// gets the struct the annotations hang off: without it the comments attach
// to nothing, and the parser reads no pool back.
func TestRender_ResourcePool_AloneKeepsItsStruct(t *testing.T) {
	tests := []struct {
		name            string
		db              *schemamodel.Database
		pools           int
		classifierCount int
	}{
		{
			name:  "a pool alone",
			db:    &schemamodel.Database{ResourcePools: []schemamodel.ResourcePool{{Name: "batch"}}},
			pools: 1,
		},
		{
			name: "a classifier alone",
			db: &schemamodel.Database{ResourcePoolClassifiers: []schemamodel.ResourcePoolClassifier{
				{Name: "everyone", Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 1}},
			}},
			classifierCount: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			files, err := goschematogo.Render(c.Context(), test.db, goschematogo.Options{
				PackageName: "models", SingleFile: true, Dialect: "ydb",
			})
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 1)

			parsed, err := goschema.ParseSource(files[0].Name, files[0].Data)

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.ResourcePools, qt.HasLen, test.pools)
			c.Assert(parsed.ResourcePoolClassifiers, qt.HasLen, test.classifierCount)
		})
	}
}
