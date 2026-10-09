package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_ResourcePool_RoundTrip writes YDB resource pools and classifiers
// as annotations that parse back to the same declarations, so `ptah
// introspect` of a YDB database keeps them: a fraction stays a fraction, and
// a pool with no setting stays one.
func TestRender_ResourcePool_RoundTrip(t *testing.T) {
	c := qt.New(t)
	want := []schemaext.Object{
		ydbworkload.DesiredPoolObject("idle", "", ydbworkload.PoolSpec{}),
		ydbworkload.DesiredPoolObject("batch", "", ydbworkload.PoolSpec{
			ConcurrentQueryLimit: new(int32(10)), QueueSize: new(int32(0)),
			DatabaseLoadCPUThreshold: new(80.5), QueryMemoryLimitPercentPerNode: new(25.0),
			QueryCPULimitPercentPerNode: new(0.001), TotalCPULimitPercentPerNode: new(100.0), ResourceWeight: new(2.25),
		}),
		ydbworkload.DesiredClassifierObject("everyone", "", ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: 0}),
		ydbworkload.DesiredClassifierObject("etl", "", ydbworkload.ClassifierSpec{ResourcePool: "batch", MemberName: "etl", Rank: 10}),
	}
	db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(want...))}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{PackageName: "models", SingleFile: true, Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	parsed, err := goschema.ParseSource(files[0].Name, files[0].Data)

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.FeatureObjects.Len(), qt.Equals, len(want))
	runtime := must.Must(builtin.New())
	original := must.Must(goschematodb.ToDBSchema(t.Context(), db, "ydb", runtime))
	again := must.Must(goschematodb.ToDBSchema(t.Context(), &parsed, "ydb", runtime))
	c.Assert(again.FeatureObjects.Equal(original.FeatureObjects), qt.IsTrue)
}

// A schema whose only global objects are pools, or only classifiers, still
// gets the struct the annotations hang off: without it the comments attach
// to nothing, and the parser reads no pool back.
func TestRender_ResourcePool_AloneKeepsItsStruct(t *testing.T) {
	for _, object := range []schemaext.Object{
		ydbworkload.DesiredPoolObject("batch", "", ydbworkload.PoolSpec{}),
		ydbworkload.DesiredClassifierObject("everyone", "", ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: 1}),
	} {
		t.Run(string(object.Value.Kind()), func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(object))}
			files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{PackageName: "models", SingleFile: true, Dialect: "ydb"})
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 1)
			parsed, err := goschema.ParseSource(files[0].Name, files[0].Data)
			c.Assert(err, qt.IsNil)
			c.Assert(parsed.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{object.Ref})
		})
	}
}
