package clickhouse_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/clickhouse"
	"ptah.run/migration/schemadiff"
)

// planningPathRecorder passes each planning request to the built-in owners
// and keeps the database path it carried.
type planningPathRecorder struct {
	featureplan.Runtime
	paths *[]string
}

func (r planningPathRecorder) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	*r.paths = append(*r.paths, request.DatabasePath)
	return r.Runtime.PlanFeatures(ctx, request)
}

// TestGenerateMigrationAST_OwnersReceiveTheDatabasePath plans a change of a
// table's storage settings and hands the owners the diff's database path.
// ClickHouse reads leave it empty, so the test sets one to see it arrive.
func TestGenerateMigrationAST_OwnersReceiveTheDatabasePath(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()", TTL: "at + INTERVAL 1 DAY"}
	after := before.Desired()
	after.TTL.Value = "at + INTERVAL 7 DAY"
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Event", Facets: must.Must(schemaext.NewFacets(after))}},
		Fields: []schemamodel.Field{{Name: "at", StructName: "Event", Type: "DateTime"}},
	}
	current := &catalog.Database{FeatureCoverage: observedStorageCoverage(), Tables: []catalog.Table{{
		Name: "events", Facets: must.Must(schemaext.NewFacets(before)),
		Columns: []catalog.Column{{Name: "at", DataType: "DateTime", ColumnType: "DateTime", IsNullable: "NO"}},
	}}}
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), source, current, "clickhouse", runtime))
	diff.CurrentDatabasePath = "/probe"
	var paths []string

	_, err := clickhouse.New().GenerateMigrationAST(t.Context(), planningPathRecorder{Runtime: runtime, paths: &paths}, diff)

	c.Assert(err, qt.IsNil)
	c.Assert(paths, qt.DeepEquals, []string{"/probe"})
}
