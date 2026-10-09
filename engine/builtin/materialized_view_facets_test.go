package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
)

// A materialized view's facets reach its renderer, which refuses a setting no
// owner of the target interprets on a view instead of rendering the view
// without it. A table's storage settings are such a setting everywhere.
func TestRenderSQL_RefusesAMaterializedViewSettingNoOwnerInterprets_FailurePath(t *testing.T) {
	for _, dialect := range []string{"postgres", "clickhouse"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			storage := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id"}
			node := &ast.CreateMaterializedViewNode{Name: "daily", Body: "SELECT 1", Facets: must.Must(schemaext.NewFacets(storage.Desired()))}
			sql, err := builtin.RenderSQL(dialect, node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
