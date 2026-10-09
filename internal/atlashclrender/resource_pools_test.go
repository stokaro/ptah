package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/atlashclrender"
)

// HCL cannot represent workload objects. Every captured value produces a loss
// diagnostic through the generic feature path, including on another target.
func TestRenderForDialect_ResourcePools(t *testing.T) {
	objects := []schemaext.Object{
		ydbworkload.DesiredPoolObject("batch", "", ydbworkload.PoolSpec{}),
		ydbworkload.DesiredClassifierObject("etl_users", "", ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 10}),
	}
	for _, dialect := range []string{"ydb", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			result, err := atlashclrender.RenderForDialect(&schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(objects...))}, dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{
				{Severity: atlashclrender.SeverityWarning, Path: `features["ptah.run/ydb/resource-pool"][""][""][""]["batch"][""]`, Message: "feature object ptah.run/ydb/resource-pool batch of kind ptah.run/ydb/resource-pool is not represented in HCL"},
				{Severity: atlashclrender.SeverityWarning, Path: `features["ptah.run/ydb/resource-pool-classifier"][""][""][""]["etl_users"][""]`, Message: "feature object ptah.run/ydb/resource-pool-classifier etl_users of kind ptah.run/ydb/resource-pool-classifier is not represented in HCL"},
			})
			empty, err := atlashclrender.RenderForDialect(&schemamodel.Database{}, dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(empty.Diagnostics, qt.HasLen, 0)
		})
	}
}
