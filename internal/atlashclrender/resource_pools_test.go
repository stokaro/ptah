package atlashclrender_test

import (
	"testing"

	"ptah.run/dialect/ydb/ydbworkload"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// A document leaves YDB resource pools and classifiers out, since Atlas HCL
// has no block for either, and says so twice: a loss diagnostic per object,
// and a header recording that it describes neither kind. A YDB document
// records both kinds whatever it holds; a document for another dialect
// records them only when its schema holds one, which a declaration written
// for YDB can.
func TestRenderForDialect_ResourcePools(t *testing.T) {
	declared := &schemamodel.Database{
		ResourcePools: []schemamodel.ResourcePool{{Name: "batch"}},
		ResourcePoolClassifiers: []schemamodel.ResourcePoolClassifier{{Name: "etl_users",
			Spec: ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 10}}},
	}
	losses := []atlashclrender.Diagnostic{
		{Severity: atlashclrender.SeverityWarning, Path: "resource_pools.batch",
			Message: "a YDB resource pool is not represented in HCL"},
		{Severity: atlashclrender.SeverityWarning, Path: "resource_pool_classifiers.etl_users",
			Message: "a YDB resource pool classifier is not represented in HCL"},
	}
	tests := []struct {
		name        string
		dialect     string
		db          *schemamodel.Database
		diagnostics []atlashclrender.Diagnostic
		recorded    bool
	}{
		{name: "a YDB document holding a pool", dialect: platform.YDB, db: declared, diagnostics: losses,
			recorded: true},
		{name: "a YDB document holding none", dialect: platform.YDB, db: &schemamodel.Database{}, recorded: true},
		{name: "a PostgreSQL document holding a pool", dialect: platform.Postgres, db: declared,
			diagnostics: losses, recorded: true},
		{name: "a PostgreSQL document holding none", dialect: platform.Postgres, db: &schemamodel.Database{},
			recorded: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := atlashclrender.RenderForDialect(test.db, test.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.DeepEquals, test.diagnostics)
			c.Assert(result.NotDescribed.Describes(coverage.ResourcePool), qt.Equals, !test.recorded)
			c.Assert(result.NotDescribed.Describes(coverage.ResourcePoolClassifier), qt.Equals, !test.recorded)
		})
	}
}
