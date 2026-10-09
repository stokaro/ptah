package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
)

// Conversion keeps a captured default pool even when all its settings are
// unset. A set-only declaration emits no operation for that empty value; the
// conversion itself must not discard a known object or its inspection limits.
func TestConvert_ResourcePools(t *testing.T) {
	for _, test := range []struct {
		name string
		spec ydbworkload.PoolSpec
	}{
		{name: "untouched default"},
		{name: "configured default", spec: ydbworkload.PoolSpec{ResourceWeight: new(30.0)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			original := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(
				ydbworkload.ObservedPoolObject("default", test.spec),
				ydbworkload.ObservedPoolObject("batch", ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(3))}),
				ydbworkload.ObservedClassifierObject("etl", ydbworkload.ClassifierSpec{ResourcePool: "batch", MemberName: "etl", Rank: 10}),
			))}
			pools := must.Must(ydbworkload.Coverage(ydbworkload.PoolKind, schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))
			classifiers := must.Must(ydbworkload.Coverage(ydbworkload.ClassifierKind, schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not enumerated"}, nil))
			original.FeatureCoverage = must.Must(pools.Combine(classifiers))
			converted, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), original, "ydb", runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(converted.FeatureObjects.Len(), qt.Equals, 3)
			object, found, err := converted.FeatureObjects.Get(ydbworkload.PoolRef("default"))
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(object.Value, qt.DeepEquals, &ydbworkload.DesiredPool{Spec: test.spec})
			c.Assert(converted.FeatureCoverage.Lookup(ydbworkload.ClassifierKind, ydbworkload.ClassifierRef("missing")).State, qt.Equals, schemaext.Uninspected)
			restored, err := goschematodb.ToDBSchema(t.Context(), converted, "ydb", runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(restored.FeatureObjects.Equal(original.FeatureObjects), qt.IsTrue)
			c.Assert(restored.FeatureCoverage.Equal(original.FeatureCoverage), qt.IsTrue)
		})
	}
}
